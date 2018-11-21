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
}

export interface ConsumerServiceOptions extends BaseServiceOptions {
}

export interface ExtendedConsumerGroupOptions extends ConsumerGroupOptions {
    commitOffsetsOnFirstJoin?: boolean
}
