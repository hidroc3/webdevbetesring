import { Module } from '@nestjs/common';
import { AwsLogsService } from './aws-logs.service';
import { AwsLogsController } from './aws-logs.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [AwsLogsController],
  providers: [AwsLogsService],
})
export class AwsLogsModule {}
