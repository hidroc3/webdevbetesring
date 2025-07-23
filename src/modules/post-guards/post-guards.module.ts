import { Module } from '@nestjs/common';
import { PostGuardsService } from './post-guards.service';
import { PostGuardsController } from './post-guards.controller';
import { PrismaModule } from '@/prisma/prisma.module';

@Module({
  imports: [PrismaModule],
  controllers: [PostGuardsController],
  providers: [PostGuardsService],
})
export class PostGuardsModule {}
