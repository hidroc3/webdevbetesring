import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateArrStationDto } from './dto/create-arr-station.dto';
import { UpdateArrStationDto } from './dto/update-arr-station.dto';

@Injectable()
export class ArrStationsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateArrStationDto) {
    return this.prisma.arrStation.create({ data });
  }

  findAll() {
    return this.prisma.arrStation.findMany();
  }

  findOne(id: number) {
    return this.prisma.arrStation.findUnique({ where: { id } });
  }

  update(id: number, data: UpdateArrStationDto) {
    return this.prisma.arrStation.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.arrStation.delete({ where: { id } });
  }

  async updateByDeviceId(
    device_id: string,
    data: Partial<UpdateArrStationDto>,
  ) {
    return this.prisma.arrStation.upsert({
      where: { device_id },
      update: data,
      create: {
        device_id,
        ...data,
      },
    });
  }
}
