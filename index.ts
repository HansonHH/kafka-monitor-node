import { ProducerService } from './lib/producer-service'

async function main() {
    const producerService = new ProducerService(
        {
            bootstrapServers: '192.168.0.102:32783,192.168.0.102:32784,192.168.0.102:32785',
            topic: 'test',
            intervalMs: 3000
        }
    )
    await producerService.start()
}

main()
