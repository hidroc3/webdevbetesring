import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateProvinceDto } from './dto/create-province.dto';
import { UpdateProvinceDto } from './dto/update-province.dto';

@Injectable()
export class ProvincesService {
  constructor(private readonly prisma: PrismaService) {}

  async create(data: CreateProvinceDto) {
    const created = await this.prisma.province.create({ data });
    return {
      id: created.id.toString(),
      name: created.name,
    };
  }

  async findAll() {
    const provinces = await this.prisma.province.findMany();
    return provinces.map((prov) => ({
      id: prov.id.toString(),
      name: prov.name,
    }));
  }

  async findOne(id: number) {
    const province = await this.prisma.province.findUnique({ where: { id } });
    if (!province) return null;

    return {
      id: province.id.toString(),
      name: province.name,
    };
  }

  async update(id: number, data: UpdateProvinceDto) {
    const updated = await this.prisma.province.update({ where: { id }, data });
    return {
      id: updated.id.toString(),
      name: updated.name,
    };
  }

  async remove(id: number) {
    const deleted = await this.prisma.province.delete({ where: { id } });
    return {
      id: deleted.id.toString(),
      name: deleted.name,
    };
  }
}
