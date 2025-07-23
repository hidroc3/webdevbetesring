import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateDasDto } from './dto/create-das.dto';
import { UpdateDasDto } from './dto/update-das.dto';

@Injectable()
export class DasService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateDasDto) {
    return this.prisma.das.create({ data });
  }

  findAll() {
    return this.prisma.das.findMany();
  }

  findOne(id: number) {
    return this.prisma.das.findUnique({ where: { id } });
  }

  update(id: number, data: UpdateDasDto) {
    return this.prisma.das.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.das.delete({ where: { id } });
  }
}
