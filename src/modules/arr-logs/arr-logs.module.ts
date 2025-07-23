import { Module } from '@nestjs/common';
import { ArrLogsService } from './arr-logs.service';
import { ArrLogsController } from './arr-logs.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  providers: [ArrLogsService],
  controllers: [ArrLogsController],
})
export class ArrLogsModule {}
