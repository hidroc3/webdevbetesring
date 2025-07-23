import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateHydrologicOutputHechmsDto } from './dto/create-hydrologic-output-hechms.dto';
import { UpdateHydrologicOutputHechmsDto } from './dto/update-hydrologic-output-hechms.dto';

@Injectable()
export class HydrologicOutputHechmsService {
  constructor(private prisma: PrismaService) {}

  create(data: CreateHydrologicOutputHechmsDto) {
    return this.prisma.hydrologicOutputHechms.create({ data });
  }

  findAll() {
    return this.prisma.hydrologicOutputHechms.findMany();
  }

  findOne(id: number) {
    return this.prisma.hydrologicOutputHechms.findUnique({ where: { id } });
  }

  update(id: number, data: UpdateHydrologicOutputHechmsDto) {
    return this.prisma.hydrologicOutputHechms.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.hydrologicOutputHechms.delete({ where: { id } });
  }
}
