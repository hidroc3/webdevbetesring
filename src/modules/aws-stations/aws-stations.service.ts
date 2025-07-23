import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateAwsStationDto } from './dto/create-aws-station.dto';
import { UpdateAwsStationDto } from './dto/update-aws-station.dto';

@Injectable()
export class AwsStationsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateAwsStationDto) {
    return this.prisma.awsStation.create({ data });
  }

  findAll() {
    return this.prisma.awsStation.findMany();
  }

  findOne(id: number) {
    return this.prisma.awsStation.findUnique({ where: { id } });
  }

  update(id: number, data: UpdateAwsStationDto) {
    return this.prisma.awsStation.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.awsStation.delete({ where: { id } });
  }
}
