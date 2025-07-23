import { Module } from '@nestjs/common';
import { SubDistrictsService } from './sub-districts.service';
import { SubDistrictsController } from './sub-districts.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [SubDistrictsController],
  providers: [SubDistrictsService],
})
export class SubDistrictsModule {}
