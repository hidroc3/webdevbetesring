import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateAwlrLogDto } from './dto/create-awlr-log.dto';
import { UpdateAwlrLogDto } from './dto/update-awlr-log.dto';

@Injectable()
export class AwlrLogsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateAwlrLogDto) {
    return this.prisma.awlrLog.create({ data });
  }

  findAll() {
    return this.prisma.awlrLog.findMany({
      orderBy: { time: 'desc' },
    });
  }

  findAllByStationId(awlr_station_id: number) {
    return this.prisma.awlrLog.findMany({
      where: { awlr_station_id },
      orderBy: { time: 'desc' },
    });
  }

  findAllToday() {
    const [start, end] = this.getTodayRange();
    return this.prisma.awlrLog.findMany({
      where: { time: { gte: start, lte: end } },
      orderBy: { time: 'desc' },
    });
  }

  findTodayByStationId(awlr_station_id: number) {
    const [start, end] = this.getTodayRange();
    return this.prisma.awlrLog.findMany({
      where: {
        awlr_station_id,
        time: { gte: start, lte: end },
      },
      orderBy: { time: 'desc' },
    });
  }

  updateByStationId(awlr_station_id: number, data: UpdateAwlrLogDto) {
    return this.prisma.awlrLog.updateMany({
      where: { awlr_station_id },
      data,
    });
  }

  removeByStationId(awlr_station_id: number) {
    return this.prisma.awlrLog.deleteMany({
      where: { awlr_station_id },
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
