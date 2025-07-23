import { Module } from '@nestjs/common';
import { VehicleTrackingLogsService } from './vehicle-tracking-logs.service';
import { VehicleTrackingLogsController } from './vehicle-tracking-logs.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [VehicleTrackingLogsController],
  providers: [VehicleTrackingLogsService],
})
export class VehicleTrackingLogsModule {}
