import { ConsumerGroupOptions } from 'kafka-node'

export interface BaseServiceOptions {
    bootstrapServers: string
    topic?: string
    id?: string
    sslOptions?: {
        ca?: string
        cert?: string
        key?: string
        rejectUnauthorized: boolean
        requestCert: boolean
    }
}

export interface ProducerServiceOptions extends BaseServiceOptions {
    recordDelayMs?: number
    recordSizeByte?: number
}

export interface ConsumerServiceOptions extends BaseServiceOptions {
    groupId?: string
}

export interface ExtendedConsumerGroupOptions extends ConsumerGroupOptions {
    commitOffsetsOnFirstJoin?: boolean
}

export interface Record {
    timestamp: number,
    dummy: string
}

export interface BrokerInfo {
    [nodeId: string]: {
        nodeId: number,
        host: string,
        port: string
    }
}

export interface TopicPartitionAssignment {
    [partition: string]: {
        topic: string
        partition: number
        leader: number
        replicas: string[]
        isr: string[]
    }
}
