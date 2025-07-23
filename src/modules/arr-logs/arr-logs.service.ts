import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateArrLogDto } from './dto/create-arr-log.dto';
import { UpdateArrLogDto } from './dto/update-arr-log.dto';

@Injectable()
export class ArrLogsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateArrLogDto) {
    return this.prisma.arrLog.create({ data });
  }

  // 🔸 Semua data hari ini (semua stasiun)
  findAllToday() {
    const [start, end] = this.getTodayRange();
    return this.prisma.arrLog.findMany({
      where: { time: { gte: start, lte: end } },
      orderBy: { time: 'desc' },
    });
  }

  // 🔹 Semua data tanpa filter waktu
  findAll() {
    return this.prisma.arrLog.findMany({
      orderBy: { time: 'desc' },
    });
  }

  // 🔸 Hari ini berdasarkan stasiun
  findTodayByStationId(arr_station_id: number) {
    const [start, end] = this.getTodayRange();
    return this.prisma.arrLog.findMany({
      where: {
        arr_station_id,
        time: { gte: start, lte: end },
      },
      orderBy: { time: 'desc' },
    });
  }

  // 🔹 Semua data berdasarkan stasiun
  findAllByStationId(arr_station_id: number) {
    return this.prisma.arrLog.findMany({
      where: { arr_station_id },
      orderBy: { time: 'desc' },
    });
  }

  updateByStationId(arr_station_id: number, data: UpdateArrLogDto) {
    return this.prisma.arrLog.updateMany({
      where: { arr_station_id },
      data,
    });
  }

  removeByStationId(arr_station_id: number) {
    return this.prisma.arrLog.deleteMany({
      where: { arr_station_id },
    });
  }

  // 🔧 Helper: Range waktu hari ini (00:00–23:59 WIB)
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
