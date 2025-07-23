import { Module } from '@nestjs/common';
import { DasService } from './das.service';
import { DasController } from './das.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [DasController],
  providers: [DasService],
})
export class DasModule {}
