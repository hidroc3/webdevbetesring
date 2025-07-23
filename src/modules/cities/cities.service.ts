import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateCityDto } from './dto/create-city.dto';
import { UpdateCityDto } from './dto/update-city.dto';

@Injectable()
export class CitiesService {
  constructor(private readonly prisma: PrismaService) {}

  async create(data: CreateCityDto) {
    return this.prisma.city.create({ data });
  }

  async findAll() {
    const cities = await this.prisma.city.findMany({
      include: {
        province: {
          select: { name: true },
        },
      },
    });

    return cities.map((city) => ({
      id: city.id.toString(),
      name: city.name,
      province: city.province?.name || null,
    }));
  }

  async findOne(id: number) {
    const city = await this.prisma.city.findUnique({
      where: { id },
      include: {
        province: {
          select: { name: true },
        },
      },
    });

    if (!city) return null;

    return {
      id: city.id.toString(),
      name: city.name,
      province: city.province?.name || null,
    };
  }

  async update(id: number, data: UpdateCityDto) {
    return this.prisma.city.update({ where: { id }, data });
  }

  async remove(id: number) {
    return this.prisma.city.delete({ where: { id } });
  }
}
