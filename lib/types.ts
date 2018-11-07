
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
    intervalMs?: number
}
