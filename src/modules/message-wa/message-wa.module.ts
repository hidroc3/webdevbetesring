import { Module } from '@nestjs/common';
import { MessageWaService } from './message-wa.service';
import { MessageWaController } from './message-wa.controller';
import { PrismaModule } from '@/prisma/prisma.module';
import { ApiWaBlastingModule } from '../api-wa-blasting/api-wa-blasting.module';

@Module({
  imports: [PrismaModule, ApiWaBlastingModule],
  controllers: [MessageWaController],
  providers: [MessageWaService],
})
export class MessageWaModule {}
