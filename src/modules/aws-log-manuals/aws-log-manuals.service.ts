import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateAwsLogManualDto } from './dto/create-aws-log-manual.dto';
import { UpdateAwsLogManualDto } from './dto/update-aws-log-manual.dto';

@Injectable()
export class AwsLogManualsService {
  constructor(private readonly prisma: PrismaService) {}

  create(data: CreateAwsLogManualDto) {
    return this.prisma.awsLogManual.create({ data });
  }

  findAll() {
    return this.prisma.awsLogManual.findMany();
  }

  findOne(id: number) {
    return this.prisma.awsLogManual.findUnique({ where: { id } });
  }

  update(id: number, data: UpdateAwsLogManualDto) {
    return this.prisma.awsLogManual.update({ where: { id }, data });
  }

  remove(id: number) {
    return this.prisma.awsLogManual.delete({ where: { id } });
  }
}
