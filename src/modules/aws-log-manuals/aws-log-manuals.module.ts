import { Module } from '@nestjs/common';
import { AwsLogManualsService } from './aws-log-manuals.service';
import { AwsLogManualsController } from './aws-log-manuals.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [AwsLogManualsController],
  providers: [AwsLogManualsService],
})
export class AwsLogManualsModule {}
