from kafka.protocol.api import Request, Response
from kafka.protocol.struct import Struct
from kafka.protocol.types import (
    Int16, Int32, Int64, Schema, String, Array, Boolean
)
from kafka.common import TopicPartition


class CreateTopicsResponse_v0(Response):
    """Response from Create Topic request (version 0)."""

    API_KEY = 19
    API_VERSION = 0
    SCHEMA = Schema(
        ('topic_errors', Array(
            ('topic', String('utf-8')),
            ('error_code', Int16))),
    )


class CreateTopicsResponse_v1(Response):
    """Response from Create Topic request (version 1)."""

    API_KEY = 19
    API_VERSION = 1
    SCHEMA = Schema(
        ('topic_errors', Array(
            ('topic', String('utf-8')),
            ('error_code', Int16),
            ('error_message', String('utf-8')))),
    )


class CreateTopicsRequest_v0(Request):
    """Request to create topic (version 0)."""

    API_KEY = 19
    API_VERSION = 0
    RESPONSE_TYPE = CreateTopicsResponse_v0
    SCHEMA = Schema(
        ('create_topic_requests', Array(
            ('topic', String('utf-8')),
            ('num_partitions', Int32),
            ('replication_factor', Int16),
            ('replica_assignment', Array(
                ('partition_id', Int32),
                ('replicas', Array(Int32)))),
            ('configs', Array(
                ('config_key', String('utf-8')),
                ('config_value', String('utf-8')))))),
        ('timeout', Int32),
    )


class CreateTopicsRequest_v1(Request):
    """Request to create topic (version 1)."""

    API_KEY = 19
    API_VERSION = 1
    RESPONSE_TYPE = CreateTopicsResponse_v1
    SCHEMA = Schema(
        ('create_topic_requests', Array(
            ('topic', String('utf-8')),
            ('num_partitions', Int32),
            ('replication_factor', Int16),
            ('replica_assignment', Array(
                ('partition_id', Int32),
                ('replicas', Array(Int32)))),
            ('configs', Array(
                ('config_key', String('utf-8')),
                ('config_value', String('utf-8')))))),
        ('timeout', Int32),
        ('validate_only', Boolean),
    )


CreateTopicsRequest = [CreateTopicsRequest_v0, CreateTopicsRequest_v1]
CreateTopicsResponse = [CreateTopicsResponse_v0, CreateTopicsRequest_v1]


class ApplicationConsumerProtocolMetadata(Struct):
    SCHEMA = Schema(
        ('version', Int16),
        ('groups', Array(
            Array(String('utf-8'))
        )))

    @property
    def subscription(self):
        return [topic for group in self.groups for topic in group]



class ApplicationConsumerProtocolAssignment(Struct):
    SCHEMA = Schema(
        ('version', Int16),
        ('assignment', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32))
        )))

    def partitions(self):
        return [TopicPartition(topic, partition)
                for topic, partitions in self.assignment # pylint: disable-msg=no-member
                for partition in partitions]


class ApplicationConsumerProtocol(object):
    PROTOCOL_TYPE = 'application'
    ASSIGNMENT_STRATEGIES = ('application')
    METADATA = ApplicationConsumerProtocolMetadata
    ASSIGNMENT = ApplicationConsumerProtocolAssignment
