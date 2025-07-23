import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreatePostHydrologicDto } from './dto/create-post-hydrologic.dto';
import { UpdatePostHydrologicDto } from './dto/update-post-hydrologic.dto';

@Injectable()
export class PostHydrologicService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreatePostHydrologicDto) {
    return this.prisma.postHydrologic.create({ data });
  }

  findAll() {
    return this.prisma.postHydrologic.findMany();
  }

  findOne(id: number) {
    return this.prisma.postHydrologic.findUnique({ where: { id } });
  }

  update(id: number, data: UpdatePostHydrologicDto) {
    return this.prisma.postHydrologic.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.postHydrologic.delete({ where: { id } });
  }
}
