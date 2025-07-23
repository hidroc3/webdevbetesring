import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateVillageDto } from './dto/create-village.dto';
import { UpdateVillageDto } from './dto/update-village.dto';

@Injectable()
export class VillagesService {
  constructor(private readonly prisma: PrismaService) {}

  async create(data: CreateVillageDto) {
    return this.prisma.village.create({ data });
  }

  async findAll() {
    const villages = await this.prisma.village.findMany({
      include: {
        subDistrict: {
          select: {
            name: true,
          },
        },
      },
    });

    return villages.map((village) => ({
      id: village.id.toString(),
      name: village.name,
      subDistrict: village.subDistrict?.name || null,
    }));
  }

  async findOne(id: number) {
    const village = await this.prisma.village.findUnique({
      where: { id },
      include: {
        subDistrict: {
          select: {
            name: true,
          },
        },
      },
    });

    if (!village) return null;

    return {
      id: village.id.toString(),
      name: village.name,
      subDistrict: village.subDistrict?.name || null,
    };
  }

  async update(id: number, data: UpdateVillageDto) {
    return this.prisma.village.update({ where: { id }, data });
  }

  async remove(id: number) {
    return this.prisma.village.delete({ where: { id } });
  }
}
