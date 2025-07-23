import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreatePostGuardDto } from './dto/create-post-guard.dto';
import { UpdatePostGuardDto } from './dto/update-post-guard.dto';

@Injectable()
export class PostGuardsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreatePostGuardDto) {
    return this.prisma.postGuard.create({ data });
  }

  findAll() {
    return this.prisma.postGuard.findMany();
  }

  findOne(id: number) {
    return this.prisma.postGuard.findUnique({ where: { id } });
  }

  update(id: number, data: UpdatePostGuardDto) {
    return this.prisma.postGuard.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.postGuard.delete({ where: { id } });
  }
}
