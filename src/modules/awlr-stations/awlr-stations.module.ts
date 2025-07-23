import { Module } from '@nestjs/common';
import { AwlrStationsService } from './awlr-stations.service';
import { AwlrStationsController } from './awlr-stations.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [AwlrStationsController],
  providers: [AwlrStationsService],
  exports: [AwlrStationsService],
})
export class AwlrStationsModule {}
