import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateHydraulicOutputHecrasDto } from './dto/create-hydraulic-output-hecras.dto';
import { UpdateHydraulicOutputHecrasDto } from './dto/update-hydraulic-output-hecras.dto';

@Injectable()
export class HydraulicOutputHecrasService {
  constructor(private prisma: PrismaService) {}

  create(data: CreateHydraulicOutputHecrasDto) {
    return this.prisma.hydraulicOutputHecras.create({ data });
  }

  findAll() {
    return this.prisma.hydraulicOutputHecras.findMany();
  }

  findOne(id: number) {
    return this.prisma.hydraulicOutputHecras.findUnique({ where: { id } });
  }

  update(id: number, data: UpdateHydraulicOutputHecrasDto) {
    return this.prisma.hydraulicOutputHecras.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.hydraulicOutputHecras.delete({ where: { id } });
  }
}
