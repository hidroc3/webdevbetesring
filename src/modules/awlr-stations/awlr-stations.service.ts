import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateAwlrStationDto } from './dto/create-awlr-station.dto';
import { UpdateAwlrStationDto } from './dto/update-awlr-station.dto';

@Injectable()
export class AwlrStationsService {
  constructor(private readonly prisma: PrismaService) {}

  // List post_name yang menggunakan rumus pertama
  private readonly firstRumusPostNames = [
    'Pabuaran',
    'Al Azhar Kaujon',
    'Pamarayan Hulu',
    'Kenari Kasunyatan',
    'Jembatan Cimake',
    'Sabagi',
    'Undar Andir',
    'Bendung Karet Cibanten',
    'Kp. Peusar',
    'Bojong Manik',
    'Cikande',
    'Jasinga',
    'Bendungan Karet Cidurian',
    'Tanjungsari',
  ];

  private calculateDebitCustom(
    water_level?: number,
    flow_a?: number,
    flow_b?: number,
    flow_ho?: number,
    post_name?: string,
  ): number | undefined {
    if (
      typeof water_level === 'number' &&
      typeof flow_a === 'number' &&
      typeof flow_b === 'number' &&
      typeof flow_ho === 'number'
    ) {
      const key = post_name?.toLowerCase() ?? '';
      const useFirstRumus = this.firstRumusPostNames.some(
        (n) => n.toLowerCase() === key,
      );

      const H = useFirstRumus ? water_level - flow_ho : water_level + flow_ho;
      if (H <= 0) return 0;

      const debit = flow_a * Math.pow(H, flow_b);
      return parseFloat(debit.toFixed(4));
    }

    return undefined;
  }

  async create(data: CreateAwlrStationDto) {
    const debit = this.calculateDebitCustom(
      data.water_level,
      data.flow_a,
      data.flow_b,
      data.flow_ho,
      data.post_name,
    );

    return this.prisma.awlrStation.create({
      data: {
        ...data,
        debit,
      },
    });
  }

  findAll() {
    return this.prisma.awlrStation.findMany();
  }

  findOne(id: number) {
    return this.prisma.awlrStation.findUnique({ where: { id } });
  }

  async update(id: number, data: UpdateAwlrStationDto) {
    const existing = await this.findOne(id);
    if (!existing) throw new Error(`AWLR Station with id ${id} not found`);

    const water_level = data.water_level ?? existing.water_level ?? undefined;
    const flow_a = data.flow_a ?? existing.flow_a ?? undefined;
    const flow_b = data.flow_b ?? existing.flow_b ?? undefined;
    const flow_ho = data.flow_ho ?? existing.flow_ho ?? undefined;
    const post_name = data.post_name ?? existing.post_name ?? undefined;

    const debit = this.calculateDebitCustom(
      water_level,
      flow_a,
      flow_b,
      flow_ho,
      post_name,
    );

    return this.prisma.awlrStation.update({
      where: { id },
      data: {
        ...data,
        debit,
      },
    });
  }

  async updateByDeviceId(
    device_id: string,
    data: Partial<UpdateAwlrStationDto>,
  ) {
    const existing = await this.prisma.awlrStation.findUnique({
      where: { device_id },
    });

    const water_level = data.water_level ?? existing?.water_level ?? undefined;
    const flow_a = data.flow_a ?? existing?.flow_a ?? undefined;
    const flow_b = data.flow_b ?? existing?.flow_b ?? undefined;
    const flow_ho = data.flow_ho ?? existing?.flow_ho ?? undefined;
    const post_name = data.post_name ?? existing?.post_name ?? undefined;

    const debit = this.calculateDebitCustom(
      water_level,
      flow_a,
      flow_b,
      flow_ho,
      post_name,
    );

    return this.prisma.awlrStation.upsert({
      where: { device_id },
      update: {
        ...data,
        debit,
      },
      create: {
        device_id,
        ...data,
        debit,
      },
    });
  }

  remove(id: number) {
    return this.prisma.awlrStation.delete({ where: { id } });
  }
}
