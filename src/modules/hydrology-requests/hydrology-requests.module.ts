import { Module } from '@nestjs/common';
import { HydrologyRequestsService } from './hydrology-requests.service';
import { HydrologyRequestsController } from './hydrology-requests.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [HydrologyRequestsController],
  providers: [HydrologyRequestsService],
})
export class HydrologyRequestsModule {}
