import struct
import time
from collections import OrderedDict
from aiokafka.record.util import encode_varint, calc_crc32c, size_of_varint

# from aiokafka.util import NO_EXTENSIONS
from kafka.codec import (
    gzip_encode, snappy_encode, lz4_encode
)

from aiokafka.record import default_records


class CompactRecordBatchBuilder(default_records._DefaultRecordBatchBuilderPy):

    def __init__(
            self, magic, compression_type, is_transactional,
            producer_id, producer_epoch, base_sequence, batch_size):
        super().__init__(self, magic, compression_type, is_transactional,
                producer_id, producer_epoch, base_sequence, batch_size)
        self._records = OrderedDict()
        self._size = len(self._buffer)

    def append(self, offset, timestamp, key, value, headers,
               # Cache for LOAD_FAST opcodes
               encode_varint=encode_varint, size_of_varint=size_of_varint,
               get_type=type, type_int=int, time_time=time.time,
               byte_like=(bytes, bytearray, memoryview),
               bytearray_type=bytearray, len_func=len, zero_len_varint=1
               ):
        """ Write message to messageset buffer with MsgVersion 2
        """
        # Check types
        if get_type(offset) != type_int:
            raise TypeError(offset)
        if timestamp is None:
            timestamp = type_int(time_time() * 1000)
        elif get_type(timestamp) != type_int:
            raise TypeError(timestamp)
        if not (key is None or get_type(key) in byte_like):
            raise TypeError(
                "Not supported type for key: {}".format(type(key)))
        if not (value is None or get_type(value) in byte_like):
            raise TypeError(
                "Not supported type for value: {}".format(type(value)))

        # We will always add the first message, so those will be set
        if self._first_timestamp is None:
            self._first_timestamp = timestamp
            self._max_timestamp = timestamp
            timestamp_delta = 0
            first_message = 1
        else:
            timestamp_delta = timestamp - self._first_timestamp
            first_message = 0

        # We can't write record right away to out buffer, we need to
        # precompute the length as first value...
        message_buffer = bytearray_type(b"\x00")  # Attributes
        write_byte = message_buffer.append
        write = message_buffer.extend

        encode_varint(timestamp_delta, write_byte)
        # Base offset is always 0 on Produce
        encode_varint(offset, write_byte)

        if key is not None:
            encode_varint(len_func(key), write_byte)
            write(key)
        else:
            write_byte(zero_len_varint)

        if value is not None:
            encode_varint(len_func(value), write_byte)
            write(value)
        else:
            write_byte(zero_len_varint)

        encode_varint(len_func(headers), write_byte)

        for h_key, h_value in headers:
            h_key = h_key.encode("utf-8")
            encode_varint(len_func(h_key), write_byte)
            write(h_key)
            if h_value is not None:
                encode_varint(len_func(h_value), write_byte)
                write(h_value)
            else:
                write_byte(zero_len_varint)

        message_len = len_func(message_buffer)

        old_message = self._records.get(key)
        if old_message is None:
            old_message_len = 0
        else:
            old_message_len = len_func(old_message)

        main_buffer = bytearray() if not first_message else self._buffer

        required_size = message_len + size_of_varint(message_len)
        # Check if we can write this message
        if (required_size + self._size - old_message_len > self._batch_size and
                not first_message):
            return None

        # Those should be updated after the length check
        if self._max_timestamp < timestamp:
            self._max_timestamp = timestamp
        if old_message is None:
            self._num_records += 1
        self._last_offset = offset
        self._size += required_size - old_message_len

        encode_varint(message_len, main_buffer.append)
        main_buffer.extend(message_buffer)

        if not first_message:
            if old_message is not None:
                del self._records[key]
            self._records[key] = main_buffer

        return _DefaultRecordMetadataPy(offset, required_size, timestamp)

    def write_records(self):
        main_buffer = self._buffer
        for message_buffer in self._records.values():
            main_buffer.extend(message_buffer)
        self._records.clear()

    def build(self):
        self.write_records()
        send_compressed = self._maybe_compress()
        self.write_header(send_compressed)
        return self._buffer

    def size(self):
        """ Return current size of data written to buffer
        """
        return self._size
