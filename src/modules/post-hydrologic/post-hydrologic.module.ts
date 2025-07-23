import { Module } from '@nestjs/common';
import { PostHydrologicService } from './post-hydrologic.service';
import { PostHydrologicController } from './post-hydrologic.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [PostHydrologicController],
  providers: [PostHydrologicService],
})
export class PostHydrologicModule {}
