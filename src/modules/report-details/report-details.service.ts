import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateReportDetailDto } from './dto/create-report-detail.dto';
import { UpdateReportDetailDto } from './dto/update-report-detail.dto';

@Injectable()
export class ReportDetailsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateReportDetailDto) {
    return this.prisma.reportDetail.create({
      data: {
        ...data,
        category_id: data.category_id ?? undefined,
      },
    });
  }

  findAll() {
    return this.prisma.reportDetail.findMany({
      include: { reportCategory: true },
    });
  }

  findOne(id: number) {
    return this.prisma.reportDetail.findUnique({
      where: { id },
      include: { reportCategory: true },
    });
  }

  update(id: number, data: UpdateReportDetailDto) {
    return this.prisma.reportDetail.update({
      where: { id },
      data: {
        ...data,
        category_id: data.category_id ?? undefined,
      },
    });
  }

  remove(id: number) {
    return this.prisma.reportDetail.delete({ where: { id } });
  }
}
