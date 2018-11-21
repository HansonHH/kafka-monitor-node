import { Record } from './types'

export function fillUpRecord(record: Record, recordSizeByte: number) {
    const recordString = JSON.stringify(record)
    const originalRecordByteLength = Buffer.byteLength(recordString)
    if (recordSizeByte > originalRecordByteLength) {
        const seed = '0'
        record.dummy = seed.repeat(recordSizeByte - originalRecordByteLength)
    }
    return JSON.stringify(record)
}
