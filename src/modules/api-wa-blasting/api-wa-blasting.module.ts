import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { ConfigModule } from '@nestjs/config';
import { ApiWaBlastingService } from './api-wa-blasting.service';
import { ApiWaBlastingController } from './api-wa-blasting.controller';

@Module({
  imports: [
    HttpModule,
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['.env'],
    }),
  ],
  providers: [ApiWaBlastingService],
  controllers: [ApiWaBlastingController],
  exports: [ApiWaBlastingService],
})
export class ApiWaBlastingModule {}
