import { Module } from '@nestjs/common';
import { PrismaModule } from '@/prisma/prisma.module';
import { LocationsSyncService } from './api-locations-sync.service';

@Module({
  imports: [PrismaModule],
  providers: [LocationsSyncService],
  exports: [LocationsSyncService],
})
export class LocationsModule {}
