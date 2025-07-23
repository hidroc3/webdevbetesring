import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateMessageWaDto } from './dto/create-message-wa.dto';
import { UpdateMessageWaDto } from './dto/update-message-wa.dto';
import { ApiWaBlastingService } from '../api-wa-blasting/api-wa-blasting.service';

@Injectable()
export class MessageWaService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly apiWaBlastingService: ApiWaBlastingService,
  ) {}

  async create(dto: CreateMessageWaDto) {
    const created = await this.prisma.messageWa.create({ data: dto });

    // Ambil semua kontak dari contact-wa
    const contacts = await this.prisma.contactWa.findMany();

    for (const contact of contacts) {
      if (contact.phoneNumber && dto.messageText) {
        await this.apiWaBlastingService
          .sendMessage({
            phoneNumber: contact.phoneNumber,
            message: dto.messageText,
          })
          .then(() => {
            console.log(`WA sent to ${contact.name}`);
          })
          .catch((error) => {
            console.error(`Failed to send WA to ${contact.name}:`, error);
          });
      }
    }

    return {
      message: 'Message saved and sent to all contacts',
      data: created,
    };
  }

  async findAll() {
    const messages = await this.prisma.messageWa.findMany();
    return {
      message: 'All messages retrieved',
      data: messages,
    };
  }

  async findOne(id: number) {
    const message = await this.prisma.messageWa.findUnique({ where: { id } });
    return {
      message: 'Message detail retrieved',
      data: message,
    };
  }

  async update(id: number, data: UpdateMessageWaDto) {
    const updated = await this.prisma.messageWa.update({
      where: { id },
      data,
    });
    return {
      message: 'Message updated',
      data: updated,
    };
  }

  async remove(id: number) {
    const deleted = await this.prisma.messageWa.delete({ where: { id } });
    return {
      message: 'Message deleted',
      data: deleted,
    };
  }
}
