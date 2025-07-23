import { Module } from '@nestjs/common';
import { ArrStationsService } from './arr-stations.service';
import { ArrStationsController } from './arr-stations.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  providers: [ArrStationsService],
  controllers: [ArrStationsController],
  exports: [ArrStationsService],
})
export class ArrStationsModule {}
