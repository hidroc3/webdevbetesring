// src/scripts/sync-locations.ts
import { NestFactory } from '@nestjs/core';
import { AppModule } from '@/app.module';
import { LocationsSyncService } from '@/modules/api-locations/api-locations-sync.service'; // ✅

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  const syncService = app.get(LocationsSyncService);

  const alreadySynced = await syncService.isLocationsSynced?.();

  if (alreadySynced) {
    console.log('✅ Data lokasi sudah ada, tidak perlu sinkronisasi ulang.');
  } else {
    console.log('🚀 Sinkronisasi data wilayah dimulai...');
    await syncService.syncLocations();
    console.log('✅ Sinkronisasi selesai.');
  }

  await app.close();
}
bootstrap();
