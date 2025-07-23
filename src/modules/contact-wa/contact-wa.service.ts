import { Injectable } from '@nestjs/common';
import { PrismaService } from '@/prisma/prisma.service';
import { CreateContactWaDto } from './dto/create-contact-wa.dto';
import { UpdateContactWaDto } from './dto/update-contact-wa.dto';
import { ApiWaBlastingService } from '../api-wa-blasting/api-wa-blasting.service';

@Injectable()
export class ContactWaService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly apiWaBlastingService: ApiWaBlastingService,
  ) {}

  async create(data: CreateContactWaDto) {
    const newContact = await this.prisma.contactWa.create({ data });

    // Contoh: Kirim pesan setelah kontak dibuat
    if (newContact.phoneNumber) {
      await this.apiWaBlastingService
        .sendMessage({
          phoneNumber: newContact.phoneNumber,
          message: `Halo ${newContact.name}, Anda telah terdaftar di database kami.`,
        })
        .then((response) => console.log('Pesan WA terkirim:', response))
        .catch((error) =>
          console.error(
            'Gagal mengirim pesan WA:',
            error.response?.data || error.message,
          ),
        );
    }
    return newContact;
  }

  async createMany(data: CreateContactWaDto[]) {
    const result = await this.prisma.contactWa.createMany({
      data,
      // skipDuplicates: false, // optional
    });

    // Contoh: Kirim pesan ke setiap kontak yang baru dibuat (opsional, tergantung kebutuhan)
    for (const contactData of data) {
      if (contactData.phoneNumber) {
        await this.apiWaBlastingService
          .sendMessage({
            phoneNumber: contactData.phoneNumber,
            message: `Halo ${contactData.name}, Anda telah ditambahkan secara massal.`,
          })
          .then((response) => console.log('Pesan WA terkirim:', response))
          .catch((error) =>
            console.error(
              'Gagal mengirim pesan WA:',
              error.response?.data || error.message,
            ),
          );
      }
    }
    return result;
  }

  findAll() {
    return this.prisma.contactWa.findMany();
  }

  findOne(id: number) {
    return this.prisma.contactWa.findUnique({ where: { id } });
  }

  async update(id: number, data: UpdateContactWaDto) {
    const updatedContact = await this.prisma.contactWa.update({
      where: { id },
      data,
    });
    // Contoh: Kirim notifikasi update
    if (updatedContact.phoneNumber) {
      await this.apiWaBlastingService
        .sendMessage({
          phoneNumber: updatedContact.phoneNumber,
          message: `Info: Data kontak Anda telah diperbarui.`,
        })
        .then((response) => console.log('Pesan WA update terkirim:', response))
        .catch((error) =>
          console.error(
            'Gagal mengirim pesan WA update:',
            error.response?.data || error.message,
          ),
        );
    }
    return updatedContact;
  }

  remove(id: number) {
    return this.prisma.contactWa.delete({ where: { id } });
  }
}
