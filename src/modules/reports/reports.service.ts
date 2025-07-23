import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateReportDto } from './dto/create-report.dto';
import { UpdateReportDto } from './dto/update-report.dto';

@Injectable()
export class ReportsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateReportDto) {
    return this.prisma.report.create({ data });
  }

  findAll() {
    return this.prisma.report.findMany({ include: { details: true } });
  }

  findOne(id: number) {
    return this.prisma.report.findUnique({
      where: { id },
      include: { details: true },
    });
  }

  update(id: number, data: UpdateReportDto) {
    return this.prisma.report.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.report.delete({ where: { id } });
  }
}
