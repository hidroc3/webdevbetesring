import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateArrLogManualDto } from './dto/create-arr-log-manual.dto';
import { UpdateArrLogManualDto } from './dto/update-arr-log-manual.dto';

@Injectable()
export class ArrLogManualsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateArrLogManualDto) {
    return this.prisma.arrLogManual.create({ data });
  }

  findAll() {
    return this.prisma.arrLogManual.findMany();
  }

  findOne(id: number) {
    return this.prisma.arrLogManual.findUnique({ where: { id } });
  }

  update(id: number, data: UpdateArrLogManualDto) {
    return this.prisma.arrLogManual.update({
      where: { id },
      data,
    });
  }

  remove(id: number) {
    return this.prisma.arrLogManual.delete({ where: { id } });
  }
}
