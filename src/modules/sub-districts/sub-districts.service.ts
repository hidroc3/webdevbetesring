import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateSubDistrictDto } from './dto/create-sub-district.dto';
import { UpdateSubDistrictDto } from './dto/update-sub-district.dto';

@Injectable()
export class SubDistrictsService {
  constructor(private readonly prisma: PrismaService) {}

  async create(data: CreateSubDistrictDto) {
    return this.prisma.subDistrict.create({ data });
  }

  async findAll() {
    const subDistricts = await this.prisma.subDistrict.findMany({
      include: {
        city: {
          select: { name: true },
        },
      },
    });

    return subDistricts.map((sd) => ({
      id: sd.id.toString(),
      name: sd.name,
      city: sd.city?.name || null,
    }));
  }

  async findOne(id: number) {
    const subDistrict = await this.prisma.subDistrict.findUnique({
      where: { id },
      include: {
        city: {
          select: { name: true },
        },
      },
    });

    if (!subDistrict) return null;

    return {
      id: subDistrict.id.toString(),
      name: subDistrict.name,
      city: subDistrict.city?.name || null,
    };
  }

  async update(id: number, data: UpdateSubDistrictDto) {
    return this.prisma.subDistrict.update({ where: { id }, data });
  }

  async remove(id: number) {
    return this.prisma.subDistrict.delete({ where: { id } });
  }
}
