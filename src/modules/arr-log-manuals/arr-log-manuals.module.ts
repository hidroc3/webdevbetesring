import { Module } from '@nestjs/common';
import { ArrLogManualsService } from './arr-log-manuals.service';
import { ArrLogManualsController } from './arr-log-manuals.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [ArrLogManualsController],
  providers: [ArrLogManualsService],
})
export class ArrLogManualsModule {}
