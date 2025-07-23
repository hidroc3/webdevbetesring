import { Module } from '@nestjs/common';
import { AwlrLogManualsService } from './awlr-log-manuals.service';
import { AwlrLogManualsController } from './awlr-log-manuals.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [AwlrLogManualsController],
  providers: [AwlrLogManualsService],
})
export class AwlrLogManualsModule {}
