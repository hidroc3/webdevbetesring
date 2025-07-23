import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateSirineDto } from './dto/create-sirine.dto';
import { UpdateSirineDto } from './dto/update-sirine.dto';

@Injectable()
export class SirineService {
  constructor(private readonly prisma: PrismaService) {}

  async findByName(name: string) {
    return this.prisma.sirine.findFirst({ where: { name } });
  }

  create(data: CreateSirineDto) {
    return this.prisma.sirine.create({ data });
  }

  createMany(data: CreateSirineDto[]) {
    return this.prisma.sirine.createMany({ data });
  }

  findAll() {
    return this.prisma.sirine.findMany();
  }

  findOne(id: bigint) {
    return this.prisma.sirine.findUnique({ where: { id } });
  }

  update(id: bigint, data: UpdateSirineDto) {
    return this.prisma.sirine.update({ where: { id }, data });
  }

  remove(id: bigint) {
    return this.prisma.sirine.delete({ where: { id } });
  }
}
