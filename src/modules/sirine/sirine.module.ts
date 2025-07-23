import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { SirineService } from './sirine.service';
import { SirineController } from './sirine.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule, HttpModule], // Tambahkan HttpModule
  controllers: [SirineController],
  providers: [SirineService],
  exports: [SirineService],
})
export class SirineModule {}
