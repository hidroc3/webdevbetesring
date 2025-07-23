import { Module } from '@nestjs/common';
import { AwlrLogsService } from './awlr-logs.service';
import { AwlrLogsController } from './awlr-logs.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [AwlrLogsController],
  providers: [AwlrLogsService],
})
export class AwlrLogsModule {}
