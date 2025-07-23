import { Module } from '@nestjs/common';
import { ContactWaService } from './contact-wa.service';
import { ContactWaController } from './contact-wa.controller';
import { PrismaModule } from '@/prisma/prisma.module';
import { ApiWaBlastingModule } from '../api-wa-blasting/api-wa-blasting.module'; // Sesuaikan path jika berbeda

@Module({
  imports: [PrismaModule, ApiWaBlastingModule],
  controllers: [ContactWaController],
  providers: [ContactWaService],
})
export class ContactWaModule {}
