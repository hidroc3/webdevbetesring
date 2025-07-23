import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateAwlrLogManualDto } from './dto/create-awlr-log-manual.dto';
import { UpdateAwlrLogManualDto } from './dto/update-awlr-log-manual.dto';

@Injectable()
export class AwlrLogManualsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateAwlrLogManualDto) {
    return this.prisma.awlrLogManual.create({ data });
  }

  findAll() {
    return this.prisma.awlrLogManual.findMany();
  }

  findOne(id: number) {
    return this.prisma.awlrLogManual.findUnique({ where: { id } });
  }

  update(id: number, data: UpdateAwlrLogManualDto) {
    return this.prisma.awlrLogManual.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.awlrLogManual.delete({ where: { id } });
  }
}
