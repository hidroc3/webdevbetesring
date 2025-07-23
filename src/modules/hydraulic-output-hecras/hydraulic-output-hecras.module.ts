import { Module } from '@nestjs/common';
import { HydraulicOutputHecrasService } from './hydraulic-output-hecras.service';
import { HydraulicOutputHecrasController } from './hydraulic-output-hecras.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [HydraulicOutputHecrasController],
  providers: [HydraulicOutputHecrasService],
})
export class HydraulicOutputHecrasModule {}
