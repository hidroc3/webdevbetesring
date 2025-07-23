import { Test, TestingModule } from '@nestjs/testing';
import { AwlrLogManualsService } from './awlr-log-manuals.service';

describe('AwlrLogManualsService', () => {
  let service: AwlrLogManualsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [AwlrLogManualsService],
    }).compile();

    service = module.get<AwlrLogManualsService>(AwlrLogManualsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
