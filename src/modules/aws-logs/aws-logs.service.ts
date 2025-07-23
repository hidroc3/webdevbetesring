import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateAwsLogDto } from './dto/create-aws-log.dto';
import { UpdateAwsLogDto } from './dto/update-aws-log.dto';

@Injectable()
export class AwsLogsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateAwsLogDto) {
    return this.prisma.awsLog.create({ data });
  }

  findAllToday() {
    const [start, end] = this.getTodayRange();
    return this.prisma.awsLog.findMany({
      where: { time: { gte: start, lte: end } },
      orderBy: { time: 'desc' },
    });
  }

  findAll() {
    return this.prisma.awsLog.findMany({
      orderBy: { time: 'desc' },
    });
  }

  findTodayByStationId(station_id: number) {
    const [start, end] = this.getTodayRange();
    return this.prisma.awsLog.findMany({
      where: {
        aws_station_id: station_id,
        time: { gte: start, lte: end },
      },
      orderBy: { time: 'desc' },
    });
  }

  findAllByStationId(station_id: number) {
    return this.prisma.awsLog.findMany({
      where: { aws_station_id: station_id },
      orderBy: { time: 'desc' },
    });
  }

  updateByStationId(station_id: number, data: UpdateAwsLogDto) {
    return this.prisma.awsLog.updateMany({
      where: { aws_station_id: station_id },
      data,
    });
  }

  removeByStationId(station_id: number) {
    return this.prisma.awsLog.deleteMany({
      where: { aws_station_id: station_id },
    });
  }

  private getTodayRange(): [Date, Date] {
    const now = new Date();
    const wibOffset = 7 * 60 * 60 * 1000;
    const wibNow = new Date(now.getTime() + wibOffset);

    const start = new Date(
      Date.UTC(
        wibNow.getUTCFullYear(),
        wibNow.getUTCMonth(),
        wibNow.getUTCDate(),
        0,
        0,
        0,
        0,
      ),
    );
    const end = new Date(
      Date.UTC(
        wibNow.getUTCFullYear(),
        wibNow.getUTCMonth(),
        wibNow.getUTCDate(),
        23,
        59,
        59,
        999,
      ),
    );

    return [start, end];
  }
}
