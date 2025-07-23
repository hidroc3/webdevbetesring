import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateHydrologyRequestDto } from './dto/create-hydrology-request.dto';
import { UpdateHydrologyRequestDto } from './dto/update-hydrology-request.dto';

@Injectable()
export class HydrologyRequestsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateHydrologyRequestDto) {
    return this.prisma.hydrologyRequest.create({ data });
  }

  findAll() {
    return this.prisma.hydrologyRequest.findMany();
  }

  findOne(id: number) {
    return this.prisma.hydrologyRequest.findUnique({ where: { id } });
  }

  update(id: number, data: UpdateHydrologyRequestDto) {
    return this.prisma.hydrologyRequest.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.hydrologyRequest.delete({ where: { id } });
  }
}
