import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateVehicleDto } from './dto/create-vehicle.dto';
import { UpdateVehicleDto } from './dto/update-vehicle.dto';
import { HttpService } from '@nestjs/axios';
import { Cron, CronExpression } from '@nestjs/schedule';
import { firstValueFrom } from 'rxjs';

@Injectable()
export class VehiclesService {
  private readonly logger = new Logger(VehiclesService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly httpService: HttpService,
  ) {}

  create(data: CreateVehicleDto) {
    return this.prisma.vehicle.create({ data });
  }

  findAll() {
    return this.prisma.vehicle.findMany({
      include: { vehicleTrackingLogs: true },
    });
  }

  findOne(id: bigint) {
    return this.prisma.vehicle.findUnique({
      where: { id },
      include: { vehicleTrackingLogs: true },
    });
  }

  update(id: bigint, data: UpdateVehicleDto) {
    return this.prisma.vehicle.update({ where: { id }, data });
  }

  remove(id: bigint) {
    return this.prisma.vehicle.delete({ where: { id } });
  }

  @Cron(CronExpression.EVERY_5_MINUTES)
  async syncFromCartrack(): Promise<void> {
    const url = 'https://fleetapi-id.cartrack.com/rest/vehicles/status';
    const auth = {
      username: 'BARU00001',
      password:
        'd5de78719399b6ef081abd9e2726f03aea69e7a10c31752bcd19c0638c08c124',
    };

    try {
      const response = await firstValueFrom(
        this.httpService.get(url, { auth }),
      );
      const vehicles = response.data?.data;

      if (!Array.isArray(vehicles)) {
        this.logger.warn('Response from Cartrack API is not an array.');
        return;
      }

      for (const v of vehicles) {
        const existing = await this.prisma.vehicle.findFirst({
          where: { registration: v.registration },
        });

        const vehicleData = {
          name: undefined,
          driver_name: undefined,
          driver_phone: undefined,
          registration: v.registration,
          chassis_number: v.chassis_number,
          time: v.event_ts ? new Date(v.event_ts) : undefined,
          latitude: v.location?.latitude,
          longitude: v.location?.longitude,
          position_description: v.location?.position_description,
          speed: v.speed,
          bearing: v.bearing,
          vehicle_status: v.ignition,
          idling: v.idling,
          odometer: v.odometer,
          altitude: v.altitude,
          fuel_level: v.fuel?.level,
          fuel_percentage: v.fuel?.precentage_left,
          created_at: new Date(),
        };

        if (existing) {
          await this.prisma.vehicle.update({
            where: { id: existing.id },
            data: vehicleData,
          });
          this.logger.log(`Updated vehicle: ${v.registration}`);
        } else {
          await this.prisma.vehicle.create({ data: vehicleData });
          this.logger.log(`Inserted new vehicle: ${v.registration}`);
        }
      }

      this.logger.log(`Synced ${vehicles.length} vehicles from Cartrack.`);
    } catch (error) {
      this.logger.error(`Error syncing Cartrack: ${error.message}`);
    }
  }
}
