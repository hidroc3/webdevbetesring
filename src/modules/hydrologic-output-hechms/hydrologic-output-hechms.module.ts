import { Module } from '@nestjs/common';
import { HydrologicOutputHechmsService } from './hydrologic-output-hechms.service';
import { HydrologicOutputHechmsController } from './hydrologic-output-hechms.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [HydrologicOutputHechmsController],
  providers: [HydrologicOutputHechmsService],
})
export class HydrologicOutputHechmsModule {}
