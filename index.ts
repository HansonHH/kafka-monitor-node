import { ConsumerService } from './lib/consumer-service'
import { ProducerService } from './lib/producer-service'

async function main() {
    const basicConfig = {
        bootstrapServers: '192.168.0.102:32783,192.168.0.102:32784,192.168.0.102:32785',
        topic: 'test'
    }
    const consumerService = new ConsumerService(basicConfig)
    const producerService = new ProducerService({
        ...basicConfig,
        intervalMs: 3000
    })
    await consumerService.isReady()
    await producerService.start()
}

main()
