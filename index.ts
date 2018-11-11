import { ConsumerService } from './lib/consumer-service'
import { ProducerService } from './lib/producer-service'

async function main() {
    const basicConfig = {
        bootstrapServers: '192.168.0.103:32797',
        topic: 'test'
    }
    const producerService = new ProducerService({
        ...basicConfig,
        intervalMs: 1000
    })
    await producerService.start()

    const consumerService = new ConsumerService(basicConfig)
    await consumerService.isReady()
}

main()
