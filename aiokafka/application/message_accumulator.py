from aiokafka.producer.message_accumulator import BatchBuilder, MessageAccumulator
from .compact_record import CompactRecordBatchBuilder
from aiokafka.record.legacy_records import LegacyRecordBatchBuilder


class CompactBatchBuilder(BatchBuilder):

    def __init__(self, magic, batch_size, compression_type,
                 *, is_transactional):
        if magic < 2:
            assert not is_transactional
            self._builder = LegacyRecordBatchBuilder(
                magic, compression_type, batch_size)
        else:
            self._builder = CompactRecordBatchBuilder(
                magic, compression_type, is_transactional=is_transactional,
                producer_id=-1, producer_epoch=-1, base_sequence=0,
                batch_size=batch_size)
        self._relative_offset = 0
        self._buffer = None
        self._closed = False

    def _set_producer_state(self, producer_id, producer_epoch, base_sequence):
        assert type(self._builder) is not LegacyRecordBatchBuilder
        self._builder.set_producer_state(
            producer_id, producer_epoch, base_sequence)


class CompactMessageAccumulator(MessageAccumulator):

    def create_builder(self):
        if self._api_version >= (0, 11):
            magic = 2
        elif self._api_version >= (0, 10):
            magic = 1
        else:
            magic = 0

        is_transactional = False
        if self._txn_manager is not None and \
                self._txn_manager.transactional_id is not None:
            is_transactional = True
        return CompactBatchBuilder(
            magic, self._batch_size, self._compression_type,
            is_transactional=is_transactional)
