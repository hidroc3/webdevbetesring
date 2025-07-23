import { Module } from '@nestjs/common';
import { ReportDetailsService } from './report-details.service';
import { ReportDetailsController } from './report-details.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [ReportDetailsController],
  providers: [ReportDetailsService],
})
export class ReportDetailsModule {}
