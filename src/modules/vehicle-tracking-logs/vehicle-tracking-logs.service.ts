import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateVehicleTrackingLogDto } from './dto/create-vehicle-tracking-log.dto';
import { UpdateVehicleTrackingLogDto } from './dto/update-vehicle-tracking-log.dto';

@Injectable()
export class VehicleTrackingLogsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateVehicleTrackingLogDto) {
    return this.prisma.vehicleTrackingLog.create({ data });
  }

  findAll() {
    return this.prisma.vehicleTrackingLog.findMany({
      include: { vehicle: true },
    });
  }

  findOne(id: number) {
    return this.prisma.vehicleTrackingLog.findUnique({
      where: { id },
      include: { vehicle: true },
    });
  }

  update(id: number, data: UpdateVehicleTrackingLogDto) {
    return this.prisma.vehicleTrackingLog.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.vehicleTrackingLog.delete({ where: { id } });
  }
}
