import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Patch,
  Delete,
} from '@nestjs/common';
import { ContactWaService } from './contact-wa.service';
import { CreateContactWaDto } from './dto/create-contact-wa.dto';
import { UpdateContactWaDto } from './dto/update-contact-wa.dto';

@Controller('contact-wa')
export class ContactWaController {
  constructor(private readonly service: ContactWaService) {}

  @Post('bulk')
  async createBulk(@Body() dtos: CreateContactWaDto[]) {
    const data = await this.service.createMany(dtos);
    return {
      message: 'Contacts successfully created in bulk',
      data,
    };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return {
      message: 'All WhatsApp contacts fetched successfully',
      data,
    };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    return {
      message: `WhatsApp contact fetched for ID ${id}`,
      data,
    };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateContactWaDto) {
    const data = await this.service.update(+id, dto);
    return {
      message: `WhatsApp contact updated for ID ${id}`,
      data,
    };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return {
      message: `WhatsApp contact deleted for ID ${id}`,
      data,
    };
  }
}
