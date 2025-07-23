import { Module } from '@nestjs/common';
import { AwsStationsService } from './aws-stations.service';
import { AwsStationsController } from './aws-stations.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [AwsStationsController],
  providers: [AwsStationsService],
})
export class AwsStationsModule {}
