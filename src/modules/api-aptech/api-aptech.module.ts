import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ScheduleModule } from '@nestjs/schedule';
import { AptechSyncService } from './api-aptech-sync.service';
import { AwlrStationsModule } from '@/modules/awlr-stations/awlr-stations.module';
import { ArrStationsModule } from '@/modules/arr-stations/arr-stations.module';

@Module({
  imports: [
    HttpModule,
    ScheduleModule.forRoot(),
    AwlrStationsModule,
    ArrStationsModule,
  ],
  providers: [AptechSyncService],
})
export class AptechModule {}
